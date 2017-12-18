<?php

namespace Drupal\workspace;

use Drupal\Core\Entity\EntityInterface;
use Drupal\Core\Entity\EntityListBuilder;
use Drupal\Core\Entity\EntityStorageInterface;
use Drupal\Core\Entity\EntityTypeInterface;
use Symfony\Component\DependencyInjection\ContainerInterface;

/**
 * Defines a class to build a listing of workspace entities.
 *
 * @see \Drupal\workspace\Entity\Workspace
 */
class WorkspaceListBuilder extends EntityListBuilder {

  /**
   * The workspace manager service.
   *
   * @var \Drupal\workspace\WorkspaceManagerInterface
   */
  protected $workspaceManager;

  /**
   * Constructs a new EntityListBuilder object.
   *
   * @param \Drupal\Core\Entity\EntityTypeInterface $entity_type
   *   The entity type definition.
   * @param \Drupal\Core\Entity\EntityStorageInterface $storage
   *   The entity storage class.
   * @param \Drupal\workspace\WorkspaceManagerInterface $workspace_manager
   *   The workspace manager service.
   */
  public function __construct(EntityTypeInterface $entity_type, EntityStorageInterface $storage, WorkspaceManagerInterface $workspace_manager) {
    parent::__construct($entity_type, $storage);
    $this->workspaceManager = $workspace_manager;
  }

  /**
   * {@inheritdoc}
   */
  public static function createInstance(ContainerInterface $container, EntityTypeInterface $entity_type) {
    return new static(
      $entity_type,
      $container->get('entity.manager')->getStorage($entity_type->id()),
      $container->get('workspace.manager')
    );
  }

  /**
   * {@inheritdoc}
   */
  public function buildHeader() {
    $header['label'] = $this->t('Workspace');
    $header['uid'] = $this->t('Owner');
    $header['status'] = $this->t('Status');

    return $header + parent::buildHeader();
  }

  /**
   * {@inheritdoc}
   */
  public function buildRow(EntityInterface $entity) {
    /** @var \Drupal\workspace\WorkspaceInterface $entity */
    $row['label'] = $entity->label() . ' (' . $entity->id() . ')';
    $row['owner'] = $entity->getOwner()->getDisplayname();
    $active_workspace = $this->workspaceManager->getActiveWorkspace();
    $row['status'] = $active_workspace == $entity->id() ? $this->t('Active') : $this->t('Inactive');

    return $row + parent::buildRow($entity);
  }

  /**
   * {@inheritdoc}
   */
  public function getDefaultOperations(EntityInterface $entity) {
    /** @var \Drupal\workspace\WorkspaceInterface $entity */
    $operations = parent::getDefaultOperations($entity);
    if (isset($operations['edit'])) {
      $operations['edit']['query']['destination'] = $entity->toUrl('collection')->toString();
    }

    $active_workspace = $this->workspaceManager->getActiveWorkspace();
    if ($entity->id() != $active_workspace) {
      $operations['activate'] = [
        'title' => $this->t('Set Active'),
        'weight' => 20,
        'url' => $entity->toUrl('activate-form', ['query' => ['destination' => $entity->toUrl('collection')->toString()]]),
      ];
    }

    if ($entity->getRepositoryHandlerPlugin()) {
      $operations['deploy'] = [
        'title' => $this->t('Deploy content'),
        'weight' => 20,
        'url' => $entity->toUrl('deploy-form', ['query' => ['destination' => $entity->toUrl('collection')->toString()]]),
      ];
    }

    return $operations;
  }

}
